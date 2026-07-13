import amqp from "amqplib";
import { clientWelcome } from "../internal/gamelogic/gamelogic.js";
import { declareAndBind, SimpleQueueType } from "../internal/pubsub/consume.js";
import { ExchangePerilDirect, PauseKey,  } from "../internal/routing/routing.js";

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

  const [channel, queue] = await  declareAndBind(conn, ExchangePerilDirect,
    `pause.${username}`, PauseKey, SimpleQueueType.Transient);
  

}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
