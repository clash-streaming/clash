import { combineReducers } from "redux";
import { historyReducer } from "./history/reducer";

export default combineReducers({
  historyReducer: historyReducer
});
