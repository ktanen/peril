import amqp from "amqplib";
import { type Channel } from "amqplib";

export enum SimpleQueueType {
  Durable,
  Transient,
}

export enum AckType {
    Ack,
    NackRequeue,
    NackDiscard,
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
        "arguments": {
            "x-dead-letter-exchange": "peril_dlx",
        },
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
  handler: (data: T) => AckType,
): Promise<void> {
    const [channel, queue] = await declareAndBind(conn, exchange, queueName, key, queueType);

    channel.consume(queue.queue, (msg: amqp.ConsumeMessage | null) => {
        if (msg === null) {
            return;
        }
        let messageContent;
        try {
            messageContent = JSON.parse(msg.content.toString());
        } catch (error) {
            if (error instanceof Error) {
                console.error(error.message);
                
            }
            channel.nack(msg, false, false)
            console.log("NackDiscard");
            return;
        }
        
        let ackType;
        try {
            ackType = handler(messageContent);
        }  catch (error) {
            if (error instanceof Error) {
                console.error(error.message);
                console.log("NackDiscard");
            }

            channel.nack(msg, false, false)
            
            return;
        }
        

        switch (ackType) {
            case AckType.Ack:
                channel.ack(msg);
                console.log("Ack");
                break;
            case AckType.NackRequeue:
                channel.nack(msg, false, true);
                console.log("NackRequeue");
                break;
            case AckType.NackDiscard:
                channel.nack(msg, false, false);
                console.log("NackDiscard");
                break;
            default:
                const unreachable: never = ackType;
                console.error("Unexpected ack type:", unreachable);
                return;
        }

    });
}