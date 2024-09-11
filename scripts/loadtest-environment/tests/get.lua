--
-- Copyright (c) 2024 - Restate Software, Inc., Restate GmbH
--
-- This file is part of the Restate load test environment,
-- which is released under the MIT license.
--
-- You can find a copy of the license in file LICENSE in the
-- scripts/loadtest-environment directory of this repository, or at
-- https://github.com/restatedev/retate/blob/main/scripts/loadtest-environment/LICENSE
--

local counter = os.time()

function setup(thread)
    thread:set("id", counter)
    counter = counter + 1
end

i = 0

request = function()
    wrk.headers["Connection"] = "Keep-Alive"
    rand_key = "key-" .. id .. "-" .. i
    i = i + 1
    --wrk.body = '3'
    --wrk.headers["Content-Type"] = "application/json"
    path = "/Counter/" .. rand_key .. "/get"
    return wrk.format("GET", path)
end
