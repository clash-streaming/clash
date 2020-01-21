import React from "react";
import { makeStyles } from "@material-ui/core/styles";
import Drawer from "@material-ui/core/Drawer";
import List from "@material-ui/core/List";
import ListItem from "@material-ui/core/ListItem";
import ListItemText from "@material-ui/core/ListItemText";
import { NavLink } from "react-router-dom";
import styles from "./SidebarStyle.js";

const useStyles = makeStyles(styles);

function Sidebar(props) {
  const classes = useStyles();

  const { logo, logoText, routes } = props;

  var links = (
    <List className={classes.list}>
      {routes.map((prop, key) => {
        return (
          <NavLink
            to={prop.path}
            className={classes.item}
            activeClassName="active"
            key={key}
          >
            <ListItem button>
              <prop.icon />
              <ListItemText primary={prop.name} />
            </ListItem>
          </NavLink>
        );
      })}
    </List>
  );

  var clashLogo = (
    <div>
      <img src={logo} alt="CLASH" />
      {logoText}
    </div>
  );
  return (
    <div>
      <Drawer variant="permanent" open>
        {clashLogo}
        <div>{links}</div>
      </Drawer>
    </div>
  );
}

export default Sidebar;
