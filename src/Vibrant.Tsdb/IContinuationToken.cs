﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public interface IContinuationToken
   {
      bool HasMore { get; }
   }
}
