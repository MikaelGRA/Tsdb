﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public interface IFieldInfo
   {
      string Key { get; }

      Type ValueType { get; }
   }
}
