import { createMuiTheme } from "@material-ui/core/styles";

export const green = {
  50: "#e7fcf5",
  100: "#c2f8e6",
  200: "#9af4d5",
  300: "#71efc4",
  400: "#52ebb7",
  500: "#34e8aa",
  600: "#2fe5a3",
  700: "#27e299",
  800: "#21de90",
  900: "#15d87f",
  A100: "#ffffff",
  A200: "#d5ffeb",
  A400: "#a2ffd2",
  A700: "#89ffc5",
  contrastDefaultColor: "dark"
};

export const yellow = {
  50: "#ffffe7",
  100: "#ffffc2",
  200: "#ffff99",
  300: "#ffff70",
  400: "#ffff52",
  500: "#ffff33",
  600: "#ffff2e",
  700: "#ffff27",
  800: "#ffff20",
  900: "#ffff14",
  A100: "#ffffff",
  A200: "#fffffb",
  A400: "#ffffc8",
  A700: "#ffffae",
  contrastDefaultColor: "dark"
};

export const blue = {
  50: "#e8f2fc",
  100: "#c6dff8",
  200: "#a0caf3",
  300: "#7ab5ee",
  400: "#5ea5eb",
  500: "#4195e7",
  600: "#3b8de4",
  700: "#3282e0",
  800: "#2a78dd",
  900: "#1c67d7",
  A100: "#ffffff",
  A200: "#d9e7ff",
  A400: "#a6c7ff",
  A700: "#8cb6ff",
  contrastDefaultColor: "dark"
};

export const red = {
  50: "#ffefe7",
  100: "#ffd7c4",
  200: "#ffbc9c",
  300: "#ffa174",
  400: "#ff8d57",
  500: "#ff7939",
  600: "#ff7133",
  700: "#ff662c",
  800: "#ff5c24",
  900: "#ff4917",
  A100: "#ffffff",
  A200: "#fffdfc",
  A400: "#ffd3c9",
  A700: "#ffbfb0",
  contrastDefaultColor: "dark"
};

export const orange = {
  50: "#fff5e7",
  100: "#ffe7c4",
  200: "#ffd79c",
  300: "#ffc674",
  400: "#ffba57",
  500: "#ffae39",
  600: "#ffa733",
  700: "#ff9d2c",
  800: "#ff9424",
  900: "#ff8417",
  A100: "#ffffff",
  A200: "#fffefc",
  A400: "#ffe1c9",
  A700: "#ffd2b0",
  contrastDefaultColor: "dark"
};

const lightTheme = createMuiTheme({
  palette: {
    type: "light",
    primary: {
      light: green[600],
      main: green[800],
      dark: green[900]
    },
    secondary: {
      light: yellow[300],
      main: yellow[500],
      dark: yellow[700]
    },
    error: red,
    background: {
      paper: "#ffffff",
      default: "#000033"
    }
  },
  typography: {
    useNextVariants: true
  }
});

// const darkTheme = createMuiTheme({
//   palette: {
//     type: "dark",
//     primary: dustyGray,
//     secondary: {
//       light: mountbattenPink[300],
//       main: mountbattenPink[500],
//       dark: mountbattenPink[700]
//     },
//     error: pharlap,
//     background: {
//       paper: "#8e8e8e",
//       default: "#333"
//     },
//     text: {
//       primary: dustyGray[50],
//       secondary: mountbattenPink[50],
//       error: pharlap[50]
//     }
//   },
//   typography: {
//     useNextVariants: true
//   }
// });

export { lightTheme };
