import React from "react";
import { ListItem, makeStyles, Typography } from "@material-ui/core";
import ListItemText from "@material-ui/core/ListItemText";

const useStyles = makeStyles(theme => ({
  item: {
    color: "red",
    backgroundColor: theme.palette.background.paper
  },
  timestamp: {
    color: "grey"
  },
  message: {
    color: "black"
  }
}));

const Message = ({ message }) => {
  //   return ;
  console.log(message);
  const classes = useStyles();

  return (
    <ListItem className={classes.item}>
      <ListItemText
        primary={message.sender}
        secondary={
          <React.Fragment>
            <Typography component="span" className={classes.timestamp}>
              {message.timestamp}
            </Typography>
            <Typography component="span" className={classes.message}>
              {message.message}
            </Typography>
          </React.Fragment>
        }
      ></ListItemText>
      <br />
    </ListItem>
  );
};

export default Message;
