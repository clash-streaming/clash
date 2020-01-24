import React from "react";

import styles from "./DashboardStyles.js";
import { makeStyles } from "@material-ui/core/styles";
import HistoryViewerContainer from "../components/HistoryViewer/HistoryViewerContainer.js";
import MessageSenderContainer from "../components/MessageSender/MessageSenderContainer.js";

const useStyles = makeStyles(styles);

function Message() {
  return (
    <div>
      <h1>Message</h1>
      <MessageSenderContainer></MessageSenderContainer>
      <HistoryViewerContainer></HistoryViewerContainer>
    </div>
  );
}

export default Message;
