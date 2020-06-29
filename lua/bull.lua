local cnt = 0
local err = 0
while ((redis.call("LLEN", "PROGRAM_LIST") ~= 0) and (cnt < PROGRAM_BATCH)) do
  local msg = redis.call("RPOP", "PROGRAM_LIST")
  if not msg then break end
  local valid_json, cmsg = pcall(cjson.decode, msg)
  if not valid_json or not cmsg or type(cmsg) ~= "table" then
    err = -2
    break
  end
  if not cmsg["$queue"] then
    cmsg["$queue"] = "PROGRAM_DEF_QUEUE"
  end
  local jqueue = cmsg["$queue"]
  cmsg["$queue"] = nil

  local pushCmd = "PROGRAM_Q_LIFO" .. "PUSH"
  local jobId = redis.call("INCR", "bull:" .. jqueue .. ":id")
  local jattempts = cmsg["$attempts"]
  local jdelay = cmsg["$delay"]
  local jobCustomId = cmsg["$jobId"]
  if not(jobCustomId == nil or jobCustomId == "") then jobId = jobCustomId end
  cmsg["$attempts"] = nil
  cmsg["$delay"] = nil
  cmsg["$jodId"] = nil
  if (type(jattempts) ~= "number") or (jattempts <= 0) then jattempts = PROGRAM_DEF_ATTEMPTS end
  if (type(jdelay) ~= "number") or (jdelay < 0) then jdelay = 0 end
  local jopt0 = cmsg["$removeOnComplete"]
  if (type(jopt0) ~= "boolean") then jopt0 = true end
  local jopts = { removeOnComplete = jopt0, delay = jdelay, attempts = jattempts }
  cmsg["$removeOnComplete"] = nil
  local jobIdKey = "bull:" .. jqueue .. ":" .. jobId
  if redis.call("EXISTS", jobIdKey) == 0 then
    redis.call("HMSET", jobIdKey, "data", cjson.encode(cmsg), "opts", cjson.encode(jopts), "progress", 0, "delay", jdelay, "timestamp", ARGV[2], "attempts", jattempts, "attemptsMade", 0, "stacktrace", "[]", "returnvalue", "null")
    if jdelay > 0 then
      local timestamp = (tonumber(ARGV[2]) + jdelay) * 0x1000 + bit.band(jobId, 0xfff)
      redis.call("ZADD", "bull:" .. jqueue .. ":delayed", timestamp, jobId)
      redis.call("PUBLISH", "bull:" .. jqueue .. ":delayed", (timestamp / 0x1000))
    else
      if redis.call("EXISTS", "bull:" .. jqueue .. ":meta-paused") ~= 1 then
        redis.call(pushCmd, "bull:" .. jqueue .. ":wait", jobId)
      else
        redis.call(pushCmd, "bull:" .. jqueue .. ":paused", jobId)
      end
      redis.call("PUBLISH", "bull:" .. jqueue .. ":waiting@null", jobId)
    end
  end
end
return {err, cnt}
