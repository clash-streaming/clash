import React from "react";
import PropTypes from "prop-types";
import List from "@material-ui/core/List";
import Timestamp from "./Timestamp";
import { Container } from "@material-ui/core";

const HistoryViewer = ({ history, reload }) => {
  const x = [];

  Object.keys(history).forEach(function(timestamp) {
    x.push(
      <Timestamp timestamp={timestamp} data={history[timestamp]}></Timestamp>
    );
  });

  return (
    <Container>
      i am a message MessageViewer
      <button onClick={() => reload()}>Click me plox</button>
      <List component="ul"></List>
      <ul>{x}</ul>
    </Container>
  );
};

HistoryViewer.propTypes = {
  history: PropTypes.objectOf(
    PropTypes.shape({
      command: PropTypes.object,
      messages: PropTypes.array
    }).isRequired
  ).isRequired,
  reload: PropTypes.func.isRequired
};
export default HistoryViewer;
