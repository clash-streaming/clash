import React from "react";
import PropTypes from "prop-types";
import { Container } from "@material-ui/core";

const MessageSender = ({ history, reload }) => {
  return <Container>i am a message MessageSender</Container>;
};

MessageSender.propTypes = {
  history: PropTypes.objectOf(
    PropTypes.shape({
      command: PropTypes.object,
      messages: PropTypes.array
    }).isRequired
  ).isRequired,
  reload: PropTypes.func.isRequired
};
export default MessageSender;
