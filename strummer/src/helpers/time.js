export function niceTime(rawTime) {
  const date = new Date(Date.parse(rawTime));
  return `${date.getHours()}:${date.getMinutes()}:${date.getSeconds()}`;
}
