config = {blocking= true}

package.path = "../lib/?.lua;;"
local beanstalk = require("nginx.beanstalk")


local client = beanstalk.new()


local retval, err = client:connect()
if not retval then
    print("connect", err)
    return 
end

print("connected")


retval, err = client:set_timeout(10)
if not retval then
    print("set_timeout", err)
    return 
end

print("timeout set")

retval, err = client:use{tube= "commit"}
if not retval then
    print("use", err)
    return
end

retval, err = client:put{data= "Hi"}
if not retval then
    print("put", err)
    return 
end

print("job id is " .. retval)

retval, err = client:watch{tube= "commit"}
if not retval then
    print("use", err)
    return 
end

print("watching ", retval, " tubes")

retval, err = client:ignore{tube= "default"}
if not retval then
    print("ignore", err)
    return 
end

retval, err = client:reserve{timeout= 0}
if not retval then
    print("reserve", err)
    return 
end

print(retval.id, retval.data)
local job_id = retval.id


retval, err = client:delete{id=job_id}
if not retval then
    print("delete", err)
    return 
end

print("job " .. job_id .. " deleted")


retval, err = client:peek{type="ready"}
if not retval then
    print("peek", err)
    return 
end

print(retval.id, retval.data)


client:quit()

print("finished")

