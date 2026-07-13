import amqp from "amqplib";
import { publishJSON } from "../internal/pubsub/publish.js";
import { PauseKey, ExchangePerilDirect } from "../internal/routing/routing.js";
import { type PlayingState } from "../internal/gamelogic/gamestate.js";

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
  const state: PlayingState = {
    isPaused: true,
  };

  try {
    await publishJSON(confirmChannel, ExchangePerilDirect, PauseKey, state);
    console.log("Message published");

  } catch (error) {
    console.log(`Publishing of pause message failed with error ${error}`);
  }



}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
