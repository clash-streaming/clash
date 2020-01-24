import React from "react";
import Message from "./Message";
import List from "@material-ui/core/List";

const Messages = ({ messages }) => {
  const x = [];

  Object.keys(messages).forEach(function(timestamp) {
    x.push(
      <Message timestamp={timestamp} message={messages[timestamp]}></Message>
    );
  });
  return <List component="ul">{x}</List>;
};

export default Messages;
