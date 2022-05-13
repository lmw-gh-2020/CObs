namespace CObs {
    public class SourceDayRoot
    {
        #pragma warning disable IDE1006

        public SourceDay? sourceDay { get; set; }
        
        #pragma warning restore IDE1006
    }

    public class SourceDay
    {
        public string? Date             { get; set; }
        public int     DNC              { get; set; }
        public int     Tests            { get; set; }
        public float   Positivity       { get; set; }
        public int     Mortality        { get; set; }
        public int     Hospitalizations { get; set; }
    }
}
