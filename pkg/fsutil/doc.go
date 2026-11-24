// Copyright 2026 Jeremy Hahn
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package fsutil makes directory changes durable: it fsyncs a directory, and
// creates a directory tree so that every directory it adds survives a crash.
//
// A file that has been fsynced is only reachable after a crash if the entry
// naming it in its parent directory is durable too, and that entry is the
// parent directory's metadata. Each directory os.MkdirAll creates therefore
// needs its parent fsynced, up to and including the deepest directory that
// already existed. MkdirAllSynced does that for a new tree. A caller that
// creates the tree itself and publishes into it before syncing records
// DeepestExistingAncestor first, then calls SyncDirChain afterwards.
//
// Every operation has a With form that takes the filesystem calls as
// parameters. A caller that routes its file I/O through its own filesystem
// abstraction, for fault injection or crash modelling, passes that
// abstraction in and gets the same walk without copying it.
package fsutil
