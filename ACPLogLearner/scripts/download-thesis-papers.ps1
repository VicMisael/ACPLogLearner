$ErrorActionPreference = 'Stop'
$destination = Join-Path $PSScriptRoot '..\docs\referencias\artigos'
New-Item -ItemType Directory -Path $destination -Force | Out-Null
$papers = @(
    @('2018-Gupta-Sadoghi-EasyCommit', 'https://openproceedings.org/2018/conf/edbt/paper-65.pdf'),
    @('2023-Pan-et-al-FLAC', 'https://arxiv.org/pdf/2302.04500'),
    @('1993-Lampson-Lomet-Presumed-Commit', 'https://www.vldb.org/conf/1993/P630.PDF'),
    @('1981-Skeen-Nonblocking-Commit', 'https://www.cs.cornell.edu/courses/cs614/2004sp/papers/Ske81.pdf'),
    @('2002-Attaluri-Salem-Presumed-Either', 'https://cs.uwaterloo.ca/~kmsalem/pubs/presumedeitherTKDE02.pdf'),
    @('2021-Jepsen-et-al-Transaction-Triaging', 'https://www.vldb.org/pvldb/vol14/p1626-lerner.pdf'),
    @('1986-Mohan-Lindsay-Obermarck-Rstar', 'https://people.csail.mit.edu/tdanford/6830papers/mohan-transaction-management.pdf'),
    @('2000-Domingos-Hulten-VFDT', 'https://alchemy.cs.washington.edu/papers/pdfs/domingos-hulten00.pdf'),
    @('2013-Gama-et-al-Evaluating-Stream-Learning', 'https://repositorio.inesctec.pt/bitstreams/435f0677-8283-40ed-9e42-503c8f38a1d6/download'),
    @('2010-Bifet-et-al-MOA', 'https://jmlr.org/papers/volume11/bifet10a/bifet10a.pdf'),
    @('2007-Bifet-Gavalda-ADWIN', 'https://www.cs.upc.edu/~Gavalda/papers/adwin06.pdf'),
    @('2009-Bifet-Gavalda-Hoeffding-Adaptive-Tree', 'https://www.cs.upc.edu/~gavalda/papers/ida09-extended.pdf'),
    @('2014-Gama-et-al-Concept-Drift-Survey', 'https://www.liaad.up.pt/area/jgama/reviewCD_methods.pdf'),
    @('2006-Gray-Lamport-Consensus-Transaction-Commit', 'https://arxiv.org/pdf/cs/0408036')
)
foreach ($paper in $papers) {
    $target = Join-Path $destination ($paper[0] + '.pdf')
    if (Test-Path -LiteralPath $target) { Write-Output "EXISTS $($paper[0])"; continue }
    & curl.exe --silent --show-error --location --fail --max-time 45 $paper[1] --output $target
    if ($LASTEXITCODE -eq 0) {
        $stream = [IO.File]::OpenRead($target)
        try { $bytes = New-Object byte[] 5; $null = $stream.Read($bytes, 0, 5) } finally { $stream.Dispose() }
        if ([Text.Encoding]::ASCII.GetString($bytes) -eq '%PDF-') { Write-Output "OK $($paper[0])" }
        else { Remove-Item -LiteralPath $target; Write-Output "INVALID $($paper[0])" }
    } else {
        if (Test-Path -LiteralPath $target) { Remove-Item -LiteralPath $target }
        Write-Output "FAILED $($paper[0])"
    }
}
