package certwatcher

// WatchedPaths returns the paths the watcher's fsnotify watcher currently watches. Test-only
// (unstructured-runtime#95): a test must wait for Start to REGISTER its watches before it changes
// the files, because an event on a file changed before its watch exists is never delivered.
func (cw *CertWatcher) WatchedPaths() []string { return cw.watcher.WatchList() }
