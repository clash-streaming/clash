import { historyQuery } from "../../api/history";

export const REQUEST_COMMAND_HISTORY = "REQUEST_COMMAND_HISTORY";
function requestCommandHistory() {
  return {
    type: REQUEST_COMMAND_HISTORY
  };
}

export const RECEIVE_COMMAND_HISTORY = "RECEIVE_COMMAND_HISTORY";
function receiveCommandHistory(json) {
  return {
    type: RECEIVE_COMMAND_HISTORY,
    rawData: json
  };
}

export function fetchCommandHistory() {
  return function(dispatch) {
    dispatch(requestCommandHistory());

    return historyQuery().then(json => dispatch(receiveCommandHistory(json)));
  };
}
