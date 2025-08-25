using System;
using System.Collections.Generic;

namespace ColumnLineageCore.Diagnostics
{
    /// <summary>
    /// Collects diagnostics during analysis, such as missing processor registrations.
    /// </summary>
    public class ProcessorDiagnostics
    {
        public HashSet<string> MissingProcessorTypes { get; } = new HashSet<string>(StringComparer.Ordinal);

        public void ReportMissingProcessor(Type fragmentType)
        {
            if (fragmentType == null) return;
            MissingProcessorTypes.Add(fragmentType.FullName ?? fragmentType.Name);
        }
    }
}
