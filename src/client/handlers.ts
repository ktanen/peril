import { type GameState, type PlayingState } from "../internal/gamelogic/gamestate.js";
import { handlePause } from "../internal/gamelogic/pause.js";
import { handleMove, MoveOutcome } from "../internal/gamelogic/move.js";
import {type ArmyMove, type RecognitionOfWar } from "../internal/gamelogic/gamedata.js";
import { AckType } from "../internal/pubsub/consume.js";
import type { ConfirmChannel } from "amqplib";
import { publishJSON } from "../internal/pubsub/publish.js";
import { WarRecognitionsPrefix, ExchangePerilTopic } from "../internal/routing/routing.js";
import { handleWar,  WarOutcome, type WarResolution } from "../internal/gamelogic/war.js";
import { publishGameLog } from "../client/index.js";
import { writeLog, type GameLog } from "../internal/gamelogic/logs.js";

export function handlerLogs(): (log: GameLog) => Promise<AckType> {
  return async (log) => {
    try {
        await writeLog(log);
        return AckType.Ack;
    } catch (error) {
        console.error("Error writing game log to disk: ", error);
        return AckType.NackDiscard;
    } finally {
        process.stdout.write("> ");
    }
  };
}
export function handlerPause(gs: GameState): (ps: PlayingState) => AckType {
    return (ps: PlayingState) => {
        handlePause(gs, ps);
        process.stdout.write("> ");
        return AckType.Ack;
    };
}

export function handlerMove(gs: GameState, ch: ConfirmChannel): (move: ArmyMove) => Promise<AckType> {

   

    return async (move: ArmyMove) => {

        try {
        const outcome = handleMove(gs, move);
        
            if (outcome === MoveOutcome.MakeWar) {
                const defendingPlayer = gs.getPlayerSnap();
                const routingKey = `${WarRecognitionsPrefix}.${defendingPlayer.username}`;
                const rw: RecognitionOfWar = {
                attacker: move.player,
                defender: defendingPlayer,
                };

                try {
                    await publishJSON(ch, ExchangePerilTopic, routingKey, rw);
                    return AckType.Ack;
                } catch(error)  {
                    console.error("Error publishing war recognition:", error);
                    return AckType.NackRequeue;
                }
                
                

            }

            
            
            if (outcome === MoveOutcome.Safe) {
                return AckType.Ack;
            } else {
                return AckType.NackDiscard;
            }
    }  finally {
        process.stdout.write("> ");
    }
    
    
    }
}


export function handlerWar(gs: GameState, ch: ConfirmChannel): (rw: RecognitionOfWar) => Promise<AckType> {
    return async (rw: RecognitionOfWar) => {
        try {
            const warResolution: WarResolution = handleWar(gs, rw);
            
            
            if (warResolution.result === WarOutcome.NotInvolved) {
                return AckType.NackRequeue;
            } else if (warResolution.result === WarOutcome.NoUnits) {
                return AckType.NackDiscard;
            } else if (warResolution.result === WarOutcome.OpponentWon || warResolution.result === WarOutcome.YouWon ||
                warResolution.result === WarOutcome.Draw) {

                    if (warResolution.result !== WarOutcome.Draw) {
                        const winner = warResolution.winner;
                        const loser = warResolution.loser;
                        const logMessage = `${winner} won a war against ${loser}`;

                        try {
                            await publishGameLog(ch, rw.attacker.username, logMessage)
                            return AckType.Ack;
                        } catch (error) {
                            console.error("Error publishing war outcome:", error);
                            return AckType.NackRequeue;
                        }

                    } else {
                        const attacker = rw.attacker.username;
                        const defender = rw.defender.username;
                        const logMessage = `A war between ${attacker} and ${defender} resulted in a draw`;
                        try {
                            await publishGameLog(ch, rw.attacker.username, logMessage)
                            return AckType.Ack;
                        } catch (error) {
                            console.error("Error publishing war outcome:", error);
                            return AckType.NackRequeue;
                        }
                    }

                    
            } else {
                console.error("Unexpected war outcome:");
                return AckType.NackDiscard;
            }

        }  finally {
            process.stdout.write("> ");
    }
    }
}