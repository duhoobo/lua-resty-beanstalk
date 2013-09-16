_G.standalone = true

package.path = "../lib/?.lua;;"
local beanstalk = require("nginx.beanstalk")


local client = beanstalk.new()


local retval, err = client:connect("127.0.0.1", 11300)
if not retval then
    print("connect", err)
    return nil
end

print("connected")

retval, err = client:set_timeout(10)
if not retval then
    print("set_timeout", err)
    return nil
end

print("timeout set")

retval, err = client:put{data= "Hi"}
if not retval then
    print("put", err)
    return nil
end

print("job id is " .. retval)

retval, err = client:reserve{timeout= 0}
if not retval then
    print("reserve", err)
    return nil
end

print(retval.id, retval.data)
local job_id = retval.id

retval, err = client:delete{id=job_id}
if not retval then
    print("delete", err)
    return nil
end

print("job " .. job_id .. " deleted")

client:quit()

print("finished")

