import { type GameState, type PlayingState } from "../internal/gamelogic/gamestate.js";
import { handlePause } from "../internal/gamelogic/pause.js";
import { handleMove, type MoveOutcome } from "../internal/gamelogic/move.js";
import {type ArmyMove } from "../internal/gamelogic/gamedata.js";

export function handlerPause(gs: GameState): (ps: PlayingState) => void {
    return (ps: PlayingState) => {
        handlePause(gs, ps);
        process.stdout.write("> ");
    };
}

export function handlerMove(gs: GameState): (move: ArmyMove) => MoveOutcome {
    return (move: ArmyMove) => {
        const outcome = handleMove(gs, move);
        process.stdout.write("> ");
        return outcome;
    }
}