import amqp from "amqplib";
import { publishJSON } from "../internal/pubsub/publish.js";
import { PauseKey, ExchangePerilDirect,
  ExchangePerilTopic, GameLogSlug } from "../internal/routing/routing.js";
import { type PlayingState } from "../internal/gamelogic/gamestate.js";
import { printServerHelp, getInput } from "../internal/gamelogic/gamelogic.js";
import { SimpleQueueType, subscribeMsgPack } from "../internal/pubsub/consume.js";
import { handlerLogs } from "../client/handlers.js";


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
  


  await subscribeMsgPack(conn, ExchangePerilTopic, GameLogSlug, `${GameLogSlug}.*`,
    SimpleQueueType.Durable, handlerLogs());

    // Used to run the server from a non-interactive source, like the multiserver.sh file
    if (!process.stdin.isTTY) {
      console.log("Non-interactive mode: skipping command input.");
      return;
    }

  printServerHelp();
  while (true) {

    const userInput = await getInput("Please enter a command:\n");

    if (userInput.length === 0) {
      continue;
    }

    const command = userInput[0];

    if (command === "pause") {
    const state: PlayingState = {
      isPaused: true,
    };
    try {
      await publishJSON(confirmChannel, ExchangePerilDirect, PauseKey, state);
      console.log("Publishing paused game state");

    } catch (error) {
      if (error instanceof Error) {
        console.error(`Publishing of pause message failed with error ${error.message}`);
      }
    }
    } else if (command === "resume") {
    const state: PlayingState = {
      isPaused: false,
    };
    try {
      await publishJSON(confirmChannel, ExchangePerilDirect, PauseKey, state);
      console.log("Publishing resumed game state");

    } catch (error) {
      if (error instanceof Error) {
        console.error(`Publishing of resume message failed with error ${error.message}`);
      }
    }
    } else if (command === "quit") {
      console.log("Exiting server");
      process.exit(0);
    } else {
      console.log("I don't understand that command.");
    }

  }




}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
