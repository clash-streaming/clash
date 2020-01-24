import React from "react";

import styles from "./DashboardStyles.js";
import { makeStyles } from "@material-ui/core/styles";
import MessageViewerContainer from "../components/MessageViewer/MessageViewerContainer.js";

const useStyles = makeStyles(styles);

function Message() {
  return (
    <div>
      <h1>Message</h1>
      <button>Load from server</button>
      <MessageViewerContainer></MessageViewerContainer>
    </div>
  );
}

export default Message;
