using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace MultiThreadingFramework
{
    class OutputDTO
    {
        private int rowNbr;
        private String data;

        public String Data
        { 
            get {return data;}
            set { data = value; }
        }
        public OutputDTO()
        {
            data = "";
            rowNbr = 0;
        }
        public OutputDTO(String sData, int iNbr)
        {
            data = sData;
            rowNbr = iNbr;
        }
        public OutputDTO(InputDTO dto)
        {
            data = dto.Data;
            rowNbr = dto.RowNbr;
        }

    }
}
