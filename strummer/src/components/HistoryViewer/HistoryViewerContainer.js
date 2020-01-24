import { connect } from "react-redux";
import HistoryViewer from "./HistoryViewer";
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

const HistoryViewerContainer = connect(
  mapStateToProps,
  mapDispatchToProps
)(HistoryViewer);

export default HistoryViewerContainer;
