package jobs

// ParserMutexRequired records the process-level parser mutex contract.
//
// Every parser child (and injected fake) must hold one process-level mutex
// for its full lifetime. This package does not import or call mrfparser.
// Queue concurrency 1 on mrf_parse is a resource bound, not that mutex.
// River 0.39 rescue changes an overdue running row to retryable or discarded;
// it does not kill the existing child.
const ParserMutexRequired = true
