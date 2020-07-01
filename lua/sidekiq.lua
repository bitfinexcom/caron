math.randomseed(tonumber(ARGV[3]))
local function get_random_string(length)
  local str = ""
  for i = 1, length do
    local rid = math.random()
    if rid < 0.3333 then
      str = str..string.char(math.random(48, 57))
    else
      str = str..string.char(math.random(97, 122))
    end
  end
  return str
end

local cnt = 0
local err = 0
local len = redis.call("LLEN", "PROGRAM_LIST")

while ((len ~= 0) and (cnt < PROGRAM_BATCH)) do
  local msg = redis.call("RPOP", "PROGRAM_LIST")
  if not msg then break end
  len = len - 1

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

  local jretry = cmsg["$retry"]
  if not jretry then jretry = false end
  if not cmsg["$class"] then cmsg["$class"] = "PROGRAM_DEF_WORKER" end
  local payload = { queue = jqueue, class = cmsg["$class"], retry = jretry }
  if (ARGV[1] == "sidekiq") then
    payload["created_at"] = ARGV[2]
    if not cmsg["$jid"] then cmsg["$jid"] = get_random_string(24) end
    payload["jid"] = cmsg["$jid"]
    if not cmsg["$backtrace"] then cmsg["$backtrace"] = false end
    payload["backtrace"] = cmsg["$backtrace"]
  end
  cmsg["$class"] = nil
  if (not cmsg["$args"]) or (type(cmsg["$args"]) ~= "table") or (next(cmsg["$args"]) == nil) then cmsg["$args"] = "ARRAY_EMPTY" end
  payload["args"] = cmsg["$args"]
  payload = cjson.encode(payload)
  payload = string.gsub(payload, '"ARRAY_EMPTY"', "[]")
  payload = string.gsub(payload, ':{}', ":null")
  redis.call("SADD", "PROGRAM_Q_PREFIX" .. "queues", jqueue)
  redis.call("LPUSH", "PROGRAM_Q_PREFIX" .. "queue:" .. jqueue, payload)
end
return {err, cnt}
