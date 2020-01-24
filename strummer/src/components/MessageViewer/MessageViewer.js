import React from "react";
import PropTypes from "prop-types";

const Message = ({ timestamp, message }) => {
  //   return ;
  return <p>{message.timestamp}</p>;
};

const Messages = ({ messages }) => {
  const x = [];

  Object.keys(messages).forEach(function(timestamp) {
    x.push(
      <li>
        <Message timestamp={timestamp} message={messages[timestamp]}></Message>
      </li>
    );
  });
  return x;
};

function makeCommand(data) {
  if ("command" in data) {
    return <p>Command</p>;
  }
  return undefined;
}

function makeMessages(data) {
  if ("messages" in data) {
    // return <Messages messages={data}></Messages>;
    return <Messages messages={data["messages"]}></Messages>;
  }
  return undefined;
}

const TimestampViewer = ({ timestamp, data }) => {
  const command = makeCommand(data);
  const messages = makeMessages(data);

  return (
    <li>
      <strong>{timestamp}</strong>
      {command}
      {messages}
    </li>
  );
};

const MessageViewer = ({ history, reload }) => {
  const x = [];

  Object.keys(history).forEach(function(timestamp) {
    x.push(
      <li>
        <TimestampViewer timestamp={timestamp} data={history[timestamp]}></TimestampViewer>
      </li>
    );
  });

  return (
    <p>
      i am a message MessageViewer
      <button onClick={() => reload()}>Click me plox</button>
      <ul>{x}</ul>
    </p>
  );
};

MessageViewer.propTypes = {
  history: PropTypes.objectOf(
    PropTypes.shape({
      command: PropTypes.object,
      messages: PropTypes.array
    }).isRequired
  ).isRequired,
  reload: PropTypes.func.isRequired
};
export default MessageViewer;
