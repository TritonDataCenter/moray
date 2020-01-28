# moray Changelog

## 2.4.0

- [#18](https://github.com/joyent/moray/issues/18) Update moray to use node-fast
  3.0.0. Node-fast version 3.0.0 moves the fast protocol version to
  version 2. Severs using node-fast 3.0.0 can still communicate with clients
  using fast protocol version 1 so updating to this version should not adversely
  impact any existing moray clients.
