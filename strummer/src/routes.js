import Dashboard from "@material-ui/icons/Dashboard";
import DashboardPage from "./views/Dashboard.js";
import Message from "@material-ui/icons/Message";
import MessagePage from "./views/Message.js";

const dashboardRoutes = [
  {
    path: "/dashboard",
    name: "Dashboard",
    icon: Dashboard,
    component: DashboardPage
  },
  {
    path: "/messages",
    name: "Messages",
    icon: Message,
    component: MessagePage
  }
];

export default dashboardRoutes;
