import amqp from "amqplib";
import { type Channel } from "amqplib";

export enum SimpleQueueType {
  Durable,
  Transient,
}

export async function declareAndBind(
  conn: amqp.ChannelModel,
  exchange: string,
  queueName: string,
  key: string,
  queueType: SimpleQueueType,
): Promise<[Channel, amqp.Replies.AssertQueue]> {
    
    const channel = await conn.createChannel();
    
    let options = {
        "durable": true,
        "autoDelete": true,
        "exclusive": true,
    };

    if (queueType === SimpleQueueType.Durable) {
        options.autoDelete = false;
        options.exclusive = false;
    } else {
        options.durable = false;
    }

    const queue = await channel.assertQueue(queueName, options);

    await channel.bindQueue(queue.queue, exchange, key)

    return [channel, queue];


}

export async function subscribeJSON<T>(
  conn: amqp.ChannelModel,
  exchange: string,
  queueName: string,
  key: string,
  queueType: SimpleQueueType,
  handler: (data: T) => void,
): Promise<void> {
    const [channel, queue] = await declareAndBind(conn, exchange, queueName, key, queueType);

    channel.consume(queue.queue, (msg: amqp.ConsumeMessage | null) => {
        if (msg === null) {
            return;
        }
        const messageContent = JSON.parse(msg.content.toString());

        handler(messageContent);

        channel.ack(msg);


    });
}