import amqp, { type ConfirmChannel } from "amqplib";
import { clientWelcome } from "../internal/gamelogic/gamelogic.js";
import { SimpleQueueType, subscribeJSON } from "../internal/pubsub/consume.js";
import { ExchangePerilDirect, PauseKey, ArmyMovesPrefix, 
  ExchangePerilTopic, WarRecognitionsPrefix, GameLogSlug } from "../internal/routing/routing.js";
import { GameState } from "../internal/gamelogic/gamestate.js";
import { getInput, commandStatus, printClientHelp, printQuit } from "../internal/gamelogic/gamelogic.js";
import { commandSpawn } from "../internal/gamelogic/spawn.js";
import { commandMove } from "../internal/gamelogic/move.js";
import { handlerPause, handlerMove, handlerWar } from "./handlers.js";
import { publishJSON, publishMsgPack } from "../internal/pubsub/publish.js";
import { type GameLog } from "../internal/gamelogic/logs.js";

export async function publishGameLog(ch: ConfirmChannel, username: string,
  message: string) {
    const currentTime = new Date();
    const log: GameLog = {
      currentTime: currentTime,
      message: message,
      username: username
    };
    await publishMsgPack(ch, ExchangePerilTopic, `${GameLogSlug}.${username}`, log);
  }

async function shutdown(conn: amqp.ChannelModel, signal: string) {
  console.log(`received ${signal}, shutting down...`);
  try {
    await conn.close();
  } catch (err) {
    console.error("error closing connection:", err);
  } finally {
    process.exit(0);
  }
}


async function main() {
  const rabbitConnString = "amqp://guest:guest@localhost:5672/";
  const conn = await amqp.connect(rabbitConnString);
  console.log("Connection successful");
  process.on("SIGINT", () => shutdown(conn, "SIGINT"));
  process.on("SIGTERM", () => shutdown(conn, "SIGTERM"));

  const confirmChannel = await conn.createConfirmChannel();
  const username = await clientWelcome();

  const moveQueueName = `${ArmyMovesPrefix}.${username}`;
  const moveKey = `${ArmyMovesPrefix}.*`;

  
  // Create new game state

  const gameState = new GameState(username);

  await subscribeJSON(conn, ExchangePerilDirect, `pause.${username}`,
    PauseKey, SimpleQueueType.Transient, handlerPause(gameState));

    await subscribeJSON(conn, ExchangePerilTopic, moveQueueName,
      moveKey, SimpleQueueType.Transient, handlerMove(gameState, confirmChannel));

    const warKey = `${WarRecognitionsPrefix}.*`;

    await subscribeJSON(conn, ExchangePerilTopic, "war", warKey, SimpleQueueType.Durable, 
      handlerWar(gameState,confirmChannel));
    

  

  // REPL loop

  while (true) {
    const userInput = await getInput("Please enter a command:\n");

    if (userInput.length === 0) {
      continue;
    }

    const command = userInput[0];


    if (command === "spawn") {
      try {
        commandSpawn(gameState, userInput);
        
      } catch (error) {
        
        if (error instanceof Error) {
          console.error(error.message);
        }
        
      }
    } else if (command === "move") {
      try {
        const move = commandMove(gameState, userInput);
        //console.log("Move successful");

        await publishJSON(confirmChannel, ExchangePerilTopic, moveKey, move);

          console.log("Move published successfully")

      } catch (error) {
        if (error instanceof Error) {
          console.error(error.message);
        }
          
      }
    } else if (command === "status") {
      await commandStatus(gameState);
      
    } else if (command === "help") {
      printClientHelp();
      
    } else if (command === "spam") {
      console.log("Spamming not allowed yet!");
    } else if (command === "quit") {
      printQuit();
      process.exit(0);
    } else {
      console.log("Unknown command");
      continue;
    }

  }

}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
