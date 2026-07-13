import{ type ConfirmChannel } from "amqplib";
import { rejects } from "assert";
import { channel } from "diagnostics_channel";

export function publishJSON<T>(
  ch: ConfirmChannel,
  exchange: string,
  routingKey: string,
  value: T,
): Promise<void> {
    const message = Buffer.from(JSON.stringify(value));
    const publishOptions = {
        "contentType": "application/json",
    };
  
  return new Promise((resolve, reject) => {
    ch.publish(exchange, routingKey, message, publishOptions, function (err, _) {
      if (err) {
        reject(err);
        return;
      }
      resolve();
    });
  });
    


    
    
}