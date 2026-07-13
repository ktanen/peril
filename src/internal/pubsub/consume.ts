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