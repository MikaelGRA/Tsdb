using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb.Redis
{
   internal static class Lua
   {
      public static readonly string PublishLatest = @"
local v = redis.call('GET', KEYS[1])
if(v) then
  if(ARGV[1] > v) then   
    redis.call('SET', KEYS[1], ARGV[1])
    return redis.call('PUBLISH', KEYS[1], ARGV[2])
  else
    return nil
  end
else
  redis.call('SET', KEYS[1], ARGV[1])
  return redis.call('PUBLISH', KEYS[1], ARGV[2])
end";
   }
}
