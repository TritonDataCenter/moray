// Copyright 2012 Joyent, Inc.  All rights reserved.

if (typeof (String.prototype.capitalize) !== 'function') {
    String.prototype.capitalize = function capitalize() {
        return this.charAt(0).toUpperCase() + this.slice(1);
    };
}
