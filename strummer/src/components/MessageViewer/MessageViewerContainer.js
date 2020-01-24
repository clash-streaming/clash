import { connect } from "react-redux";
import MessageViewer from "./MessageViewer";
import { fetchCommandHistory } from "../../store/history/actions";

const mapStateToProps = state => {
  return { history: state.historyReducer.history };
};

const mapDispatchToProps = dispatch => {
  return {
    reload: () => {
      dispatch(fetchCommandHistory());
    }
  };
};

const MessageViewerContainer = connect(mapStateToProps, mapDispatchToProps)(MessageViewer);

export default MessageViewerContainer;
