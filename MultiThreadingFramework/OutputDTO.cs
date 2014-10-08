using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace MultiThreadingFramework
{
    class OutputDTO
    {
        public int RowNbr {get; set;}
        private String data;

        public String Data
        { 
            get {return data;}
            set { data = value; }
        }
        public OutputDTO()
        {
            data = "";
            RowNbr = 0;
        }
        public OutputDTO(String sData, int iNbr)
        {
            data = sData;
            RowNbr = iNbr;
        }
        public OutputDTO(InputDTO dto)
        {
            data = dto.Data;
            RowNbr = dto.RowNbr;
        }

    }
}
