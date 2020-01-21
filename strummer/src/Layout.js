import React from "react";
import logo from "./logo.svg";
import "./App.css";
import Sidebar from "./components/Sidebar";
import routes from "./routes.js";
import { Switch, Route, Redirect } from "react-router-dom";

const switchRoutes = (
  <Switch>
    {routes.map((prop, key) => {
      return <Route path={prop.path} component={prop.component} key={key} />;
    })}
    <Redirect from="/" to="/dashboard" />
  </Switch>
);

function Layout() {
  var image = "foo";
  return (
    <div className="App">
      <Sidebar
        routes={routes}
        logoText={"CLASH Manager"}
        logo={logo}
        image={image}
      ></Sidebar>
      <div>
        {/* <div className={classes.map}> */}
        {switchRoutes}
      </div>
    </div>
  );
}

export default Layout;
