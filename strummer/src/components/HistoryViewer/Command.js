import React from "react";

const Command = ({ command }) => {
  console.log(command);
  return <p>{command.command}</p>;
};

export default Command;
