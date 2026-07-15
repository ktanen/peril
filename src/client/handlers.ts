import { type GameState, type PlayingState } from "../internal/gamelogic/gamestate.js";
import { handlePause } from "../internal/gamelogic/pause.js";
import { handleMove, MoveOutcome } from "../internal/gamelogic/move.js";
import {type ArmyMove } from "../internal/gamelogic/gamedata.js";
import { AckType } from "../internal/pubsub/consume.js";

export function handlerPause(gs: GameState): (ps: PlayingState) => AckType {
    return (ps: PlayingState) => {
        handlePause(gs, ps);
        process.stdout.write("> ");
        return AckType.Ack;
    };
}

export function handlerMove(gs: GameState): (move: ArmyMove) => AckType {
    return (move: ArmyMove) => {
        const outcome = handleMove(gs, move);


        process.stdout.write("> ");
        
        if (outcome === MoveOutcome.Safe || outcome === MoveOutcome.MakeWar) {
            return AckType.Ack;
        } else {
            return AckType.NackDiscard;
        }
    }
}