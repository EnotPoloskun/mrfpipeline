package jobs

// ParserMutexRequired records the Story 10 correctness contract.
//
// Every mrfparser.Parse call must hold one process-level mutex. This package
// does not import or call mrfparser. Queue concurrency 1 on mrf_parse is a
// resource bound, not that mutex. River 0.39 rescue changes an overdue running
// row to retryable or discarded; it does not kill the existing Go invocation.
const ParserMutexRequired = true
