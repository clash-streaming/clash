import { REQUEST_COMMAND_HISTORY, RECEIVE_COMMAND_HISTORY } from "./actions";

export function historyReducer(
  state = {
    requesting: false,
    history: {}
  },
  action
) {
  switch (action.type) {
    case REQUEST_COMMAND_HISTORY:
      return Object.assign({}, state, {
        requesting: true
      });
    case RECEIVE_COMMAND_HISTORY:
      return Object.assign({}, state, {
        requesting: false,
        history: action.rawData
      });
    default:
      return state;
  }
}
