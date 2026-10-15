# Artigos da dissertação ALPS

Downloads realizados em 2026-10-13. Os nomes dos PDFs indicam o ano da publicação citada; versões de autores e preprints podem ter datas diferentes.

| PDF | Fonte |
| --- | --- |
| 2018-Gupta-Sadoghi-EasyCommit.pdf | https://openproceedings.org/2018/conf/edbt/paper-65.pdf |
| 2023-Pan-et-al-FLAC.pdf | https://arxiv.org/pdf/2302.04500 |
| 1981-Skeen-Nonblocking-Commit.pdf | https://www.cs.cornell.edu/courses/cs614/2004sp/papers/Ske81.pdf |
| 2002-Attaluri-Salem-Presumed-Either.pdf | https://cs.uwaterloo.ca/~kmsalem/pubs/presumedeitherTKDE02.pdf |
| 2021-Jepsen-et-al-Transaction-Triaging.pdf | https://www.vldb.org/pvldb/vol14/p1626-lerner.pdf |
| 1986-Mohan-Lindsay-Obermarck-Rstar.pdf | https://people.csail.mit.edu/tdanford/6830papers/mohan-transaction-management.pdf |
| 2000-Domingos-Hulten-VFDT.pdf | https://alchemy.cs.washington.edu/papers/pdfs/domingos-hulten00.pdf |
| 2010-Bifet-et-al-MOA.pdf | https://jmlr.org/papers/volume11/bifet10a/bifet10a.pdf |
| 2007-Bifet-Gavalda-ADWIN.pdf | https://www.cs.upc.edu/~Gavalda/papers/adwin06.pdf |
| 2009-Bifet-Gavalda-Hoeffding-Adaptive-Tree.pdf | https://www.cs.upc.edu/~gavalda/papers/ida09-extended.pdf |
| 2006-Gray-Lamport-Consensus-Transaction-Commit.pdf | https://arxiv.org/pdf/cs/0408036 |

## Referências copiadas da coleção local

Copiadas de `D:\Dev\books\artigos` em 2026-10-13, preservando os originais e verificando igualdade por SHA-256. Seleção focada em commit adaptativo, otimizações de presumed commit e interação entre consenso e commit atômico. A tese foi incluída como leitura complementar. Anos omitidos nos nomes quando ainda não verificados.

| Arquivo no projeto | Origem relativa à coleção local |
| --- | --- |
| 1993-Lampson-Lomet-Presumed-Commit.pdf | ler/lampson-2pc.pdf |
| Dynamic-Two-Phase-Commit-Self-Adapting-Services.pdf | a-dynamic-twophase-commit-protocol-for-selfadapting-services.pdf |
| Pan-et-al-FC-Adaptive-Atomic-Commit-Failure-Detection.pdf | ler/FC_Adaptive_Atomic_Commit_via_Failure_Detection_250729_130337.pdf |
| 2004-Al-Houmaily-Chrysanthis-One-Two-Phase-Commit.pdf | Raft as a Atomic Commit Protocol/al-houmaily2004.pdf |
| Efficient-Distributed-Transaction-Processing-Heterogeneous-Networks.pdf | p1372-lu.pdf |
| 1997-Al-Houmaily-et-al-Enhancing-Presumed-Commit.pdf | DB-Distribuidos/PAXOS_CAP/PAXOS_CAP/1997_SAC_p131-al-houmaily.pdf |
| Maiyya-et-al-Unifying-Consensus-Atomic-Commitment.pdf | Raft as a Atomic Commit Protocol/p611-maiyya.pdf |
| 2025-Eldeeb-Optimizing-Distributed-Transactions-Thesis.pdf | Eldeeb_columbia_0054D_19286.pdf |

EasyCommit, FLAC e Consensus on Transaction Commit já estavam presentes no projeto e não foram duplicados. O arquivo `FLAC.pdf` da raiz da coleção local contém outro artigo e não foi copiado.

## Downloads pendentes

- Gama, Sebastião e Rodrigues (2013), On Evaluating Stream Learning Algorithms: https://repositorio.inesctec.pt/bitstreams/435f0677-8283-40ed-9e42-503c8f38a1d6/download — retornou HTML em vez de PDF.
- Gama et al. (2014), A Survey on Concept Drift Adaptation: https://www.liaad.up.pt/area/jgama/reviewCD_methods.pdf — certificado do servidor inválido para o endereço acessado. Página da publicação: https://doi.org/10.1145/2523813.

O livro Principles of Distributed Database Systems (Özsu e Valduriez, 2019) não foi baixado. Acesso pela editora ou biblioteca institucional: https://link.springer.com/book/10.1007/978-3-030-26253-2.

Para repetir os downloads pendentes, execute `powershell.exe -NoProfile -ExecutionPolicy Bypass -File scripts/download-thesis-papers.ps1` na raiz do projeto. O script preserva os arquivos existentes.
