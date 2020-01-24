import { connect } from "react-redux";
import MessageSender from "./MessageSender";

const mapStateToProps = state => {
  return {};
};

const mapDispatchToProps = dispatch => {
  return {};
};

const MessageSenderContainer = connect(
  mapStateToProps,
  mapDispatchToProps
)(MessageSender);

export default MessageSenderContainer;
