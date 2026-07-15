import amqp from "amqplib";
import { clientWelcome } from "../internal/gamelogic/gamelogic.js";
import { declareAndBind, SimpleQueueType, subscribeJSON } from "../internal/pubsub/consume.js";
import { ExchangePerilDirect, PauseKey  } from "../internal/routing/routing.js";
import { GameState } from "../internal/gamelogic/gamestate.js";
import { getInput, commandStatus, printClientHelp, printQuit } from "../internal/gamelogic/gamelogic.js";
import { commandSpawn } from "../internal/gamelogic/spawn.js";
import { commandMove } from "../internal/gamelogic/move.js";
import { handlerPause } from "./handlers.js";

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
  const username = await clientWelcome();
  await  declareAndBind(conn, ExchangePerilDirect,
  `pause.${username}`, PauseKey, SimpleQueueType.Transient);
  
  // Create new game state

  const gameState = new GameState(username);

  await subscribeJSON(conn, ExchangePerilDirect, `pause.${username}`,
    PauseKey, SimpleQueueType.Transient, handlerPause(gameState));
  

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
