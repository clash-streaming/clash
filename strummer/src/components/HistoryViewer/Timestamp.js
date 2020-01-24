import React from "react";
import ListItem from "@material-ui/core/ListItem";
import Messages from "./Messages";
import { ListItemText } from "@material-ui/core";
import Command from "./Command";

function makeCommand(data) {
  if ("command" in data) {
    return <Command command={data.command}></Command>;
  }
  return undefined;
}

function makeMessages(data) {
  if ("messages" in data) {
    return <Messages messages={data.messages}></Messages>;
  }
  return undefined;
}

const Timestamp = ({ timestamp, data }) => {
  const command = makeCommand(data);
  const messages = makeMessages(data);

  return (
    <ListItem>
      <ListItemText
        primary={timestamp}
        secondary={
          <React.Fragment>
            {command}
            {messages}
          </React.Fragment>
        }
      />
    </ListItem>
  );
};

export default Timestamp;
