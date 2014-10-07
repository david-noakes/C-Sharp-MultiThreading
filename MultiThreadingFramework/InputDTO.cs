using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace MultiThreadingFramework
{
    class InputDTO
    {
        // simple input record

        public String Data { get; set; }
        public int RowNbr { get; set; }

        public InputDTO()
        {
            Data = "";
            RowNbr = 0;
        }
        public InputDTO(String data, int rowNum)
        {
            Data = data;
            RowNbr = rowNum;
        }

    }
}
