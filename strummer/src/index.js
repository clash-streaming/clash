import React from "react";
import ReactDOM from "react-dom";
import { createBrowserHistory } from "history";
import "./index.css";
import * as serviceWorker from "./serviceWorker";
import { Router, Route, Switch, Redirect } from "react-router-dom";
import Layout from "./Layout";
import { lightTheme } from "./Themes";
import { MuiThemeProvider } from "@material-ui/core/styles";

const hist = createBrowserHistory();

ReactDOM.render(
  <MuiThemeProvider theme={lightTheme}>
    <Router history={hist}>
      <Switch>
        <Route path="/" component={Layout} />
        <Redirect from="/" to="/dashboard" />
      </Switch>
    </Router>
  </MuiThemeProvider>,
  document.getElementById("root")
);

// If you want your app to work offline and load faster, you can change
// unregister() to register() below. Note this comes with some pitfalls.
// Learn more about service workers: https://bit.ly/CRA-PWA
serviceWorker.unregister();
