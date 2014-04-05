-- Copyleft (C) 2013 Live Long and Prosper

local tcp

if config and config.blocking then
    local socket = require("socket.core")
    tcp = socket.tcp 
else
    tcp = ngx.socket.tcp
end

local sub = string.sub
local gsub = string.gsub
local format = string.format
local match = string.match
local strlen = string.len
local insert = table.insert
local concat = table.concat
local remove = table.remove
local setmetatable = setmetatable
local type = type
local error = error
local pairs = pairs
local print = print
local tonumber = tonumber


module(...)

_VERSION = "0.01"

local DEFAULT_HOST = "localhost"
local DEFAULT_PORT = 11300

local DEFAULT_TUBE = "default"
local DEFAULT_PRIORITY = 2 ^ 32 - 1
local DEFAULT_DELAY = 0
local DEFAULT_TTR = 120



local function _split(line, sep)
    local sep, fields = sep or ":", {}
    local pat = format("([^%s]+)", sep)

    gsub(line, pat, function(c) 
        fields[#fields+1] = c 
    end)

    return fields
end


local function _readable_error(indicator)
    local errors = {
        ["OUT_OF_MEMORY"]= "out of memory",
        ["INTERNAL_ERROR"]= "internel error",
        ["BAD_FORMAT"]= "bad format",
        ["UNKNOWN_COMMAND"]= "unknown command",
        ["EXPECTED_CRLF"]= "expect CRLF",
        ["JOB_TOO_BIT"]= "job too big",
        ["DRAINING"]= "server in drain mode",
        ["DEADLINE_SOON"]= "deadline soon",
        ["TIMED_OUT"]= "timedout",
        ["NOT_FOUND"]= "job not found",
    }

    for key, msg in pairs(errors) do
        if key == indicator then return msg end
    end

    return indicator
end


local function _interact(sock, request, expected)
    local bytes, err = sock:send(request)
    if not bytes then
        return nil, "send failed: " .. err
    end

    local line, err, partial = sock:receive("*l")
    if not line then
        return nil, "read failed: " .. err
    end

    local parts = _split(line, " ")

    for _, indicator in pairs(expected) do
        if parts[1] == indicator then
            remove(parts, 1)
            return parts
        end
    end

    return nil, _readable_error(parts[1])
end


local function _read_job(sock, jid, bytes)
    if not tonumber(jid) or not tonumber(bytes) 
    then
        return nil, "jid and bytes not number"
    end

    local result = {id= tonumber(jid)}

    local line, err = sock:receive(bytes)
    if not line then
        return nil, "read job data failed: " .. (err or "")
    end

    result.data = line

    line, err = sock:receive(2)
    if not line then
        return nil, "read last CRLF failed: " .. (err or "")
    end

    return result
end



--
-- public methods
--
function new(self)
    local sock, err = tcp()
    if not sock then
        return nil, err
    end

    return setmetatable({sock = sock}, {__index = _M})
end


function set_timeout(self, ...)
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end

    return sock:settimeout(...)
end


function set_keepalive(self, ...)
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end

    return sock:setkeepalive(...)
end


function connect(self, args)
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end

    if args and type(args) ~= "table" then
        return nil, "args should be a table"
    end

    local host = args and args.host or DEFAULT_HOST
    local port = args and args.port or DEFAULT_PORT

    return sock:connect(host, port)
end


function put(self, args)
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end

    if type(args) ~= "table" then
        return nil, "args should be a table"
    end

    if type(args.data) ~= "string" then
        return nil, "payload should be string"
    end

    args.priority = args.priority or DEFAULT_PRIORITY
    args.ttr = args.ttr or DEFAULT_TTR
    args.delay = args.delay or DEFAULT_DELAY

    local reply, err = _interact(sock, concat{
            "put ", args.priority, " ", args.delay, " ", 
                args.ttr, " ", #args.data, "\r\n",
                args.data, "\r\n"
        }, {"INSERTED", "BURIED"})

    if not reply then
        return nil, err
    end

    return reply[1]
end


function use(self, args)
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end

    if type(args) ~= "table" then
        return nil, "args should be a table"
    end

    args.tube = args.tube or DEFAULT_TUBE

    local retval, err = _interact(sock, concat{
            "use ", args.tube, "\r\n"
        }, {"USING"})

    if not retval then
        return nil, err
    end

    return retval[1]
end


function reserve(self, args)
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end

    if args and type(args) ~= "table" then
        return nil, "args should be a table"
    end

    local request

    if args and args.timeout then
        request = concat{"reserve-with-timeout ", args.timeout, "\r\n"}
    else
        request = "reserve\r\n"
    end

    local retval, err = _interact(sock, request, {"RESERVED"})
    if not retval then
        return nil, err
    end

    retval, err = _read_job(sock, retval[1], retval[2])
    if not retval then
        return nil, err
    end

    return retval
end


function delete(self, args)
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end

    if type(args) ~= "table" then
        return nil, "args should be a table"
    end

    if type(args.id) ~= "number" then
        return nil, "job id should be a number"
    end

    local retval, err = _interact(sock, concat{
            "delete ", args.id, "\r\n"
        }, {"DELETED"})
    if not retval then
        return nil, err
    end

    return 1
end


function release(self, args)
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end

    if type(args) ~= "table" then
        return nil, "args should be a table"
    end

    if type(args.id) ~= "number" then
        return nil, "job id should be a number"
    end

    args.priority = args.priority or DEFAULT_PRIORITY
    args.delay = args.delay or DEFAULT_DELAY

    local retval, err = _interact(sock, concat{
            "release ", args.id, " ", args.priority, " ",
                        args.delay, "\r\n"
        }, {"RELEASED", "BURIED"})
    if not retval then
        return nil, err
    end

    return 1
end


function bury(self, args)
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end

    if type(args) ~= "table" then
        return nil, "args should be a table"
    end

    if type(args.id) ~= "number" then
        return nil, "job id should be a number"
    end

    args.priority = args.priority or DEFAULT_PRIORITY

    local retval, err = _interact(sock, concat{
            "bury ", args.id, " ", args.priority, "\r\n"
        }, {"BURIED"})
    if not retval then
        return nil, err
    end

    return 1
end


function touch(self, args)
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end

    if type(args) ~= "table" then
        return nil, "args should be a table"
    end

    if type(args.id) ~= "number" then
        return nil, "job id should be a number"
    end

    local retval, err = _interact(sock, concat{
            "touch", args.id, "\r\n"
        }, {"TOUCHED"})
    if not retval then
        return nil, err
    end

    return 1
end


function watch(self, args)
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end

    if type(args) ~= "table" then
        return nil, "args should be a table"
    end

    args.tube = args.tube or "default"

    local reply, err = _interact(sock, concat{
            "watch ", args.tube, "\r\n"
        }, {"WATCHING"})
    
    if not reply then
        return nil, err
    end

    return reply[1]
end


function ignore(self, args)
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end

    if type(args) ~= "table" then
        return nil, "args should be a table"
    end

    args.tube = args.tube or "default"

    local reply, err = _interact(sock, concat{
            "ignore ", args.tube, "\r\n"
        }, {"WATCHING", "NOT_IGNORED"})
    
    if not reply then
        return nil, err
    end

    return reply[1] and reply[1] or 1
end


local _peek_types = {
    ready= "-ready", delayed= "-delayed", buried= "-buried"
}

function peek(self, args)
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end

    if type(args) ~= "table" then
        return nil, "args should be a table"
    end

    if args.id and type(args.id) ~= "number" then
        return nil, "job is should be a number"
    end

    if not args.id and not _peek_types[args.type] then
        return nil, "wrong peek variation"
    end

    local request = {}
    if args.id then
        request = concat{"peek ", args.id, "\r\n"}
    else
        request = concat{"peek", _peek_types[args.type], "\r\n"}
    end

    local retval, err = _interact(sock, request, {"FOUND"})
    if not retval then
        return nil, err
    end

    retval, err = _read_job(sock, retval[1], retval[2])
    if not retval then
        return nil, err
    end

    return retval
end


function kick(self, args)
    return nil, "not implemented"
end

function stats(self, args)
    return nil, "not implemented"
end

function list(self, args)
    return nil, "not implemented"
end


function quit(self)
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end

    sock:send("quit\r\n")

    sock:close() 
end


local class_mt = {
    -- to prevent use of casual module global variables
    __newindex = function (table, key, val)
        error('attempt to write to undeclared variable "' .. key .. '"')
    end
}

setmetatable(_M, class_mt)

