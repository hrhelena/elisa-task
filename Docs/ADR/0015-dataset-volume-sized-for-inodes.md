# 0015. Size the dataset volume at 10Gi for inode capacity

- **Status:** Accepted
- **Date:** 2026-10-23
- **Author:** Helena Rivas (with product-owner agent)
- **Deciders:** Helena Rivas
- **Related:** [#10](https://github.com/hrhelena/elisa-task/issues/10), [#12](https://github.com/hrhelena/elisa-task/issues/12), [PR #24](https://github.com/hrhelena/elisa-task/pull/24), amends [0014](0014-kubernetes-deployment-topology.md)

## Summary

The dataset PVC `enron-data` defaults to 10Gi instead of the 5Gi in [0014](0014-kubernetes-deployment-topology.md),
so that a default ext4 volume has enough inodes for the extracted dataset. Cloud overlays may override the size.
Only the PVC size in 0014 changes; the rest of 0014 stands.

## Context

- The extracted Enron maildir is about 2.5 GB, but it has about 520,901 files and directories, so it needs about
  521k inodes.
- The default storage classes on EKS (gp3) and GKE (standard-rwo) format volumes as ext4, which allocates about one
  inode per 16 KiB: 5Gi gives about 327k inodes (too few), and 10Gi gives about 655k.
- Since PR #24 (#10), `POST /fetch` fails fast with `insufficient free inodes` when `DATA_DIR` has fewer than
  530,000 free inodes (and with `insufficient disk space` below 4 GiB free). With a 5Gi ext4 volume, every cloud
  fetch would fail immediately.
- kind's local-path provisioner uses a host directory, so its inode count does not depend on the PVC size.

## Decision

- PVC `enron-data` in `deploy/k8s/base`: `ReadWriteOnce`, `storage: 10Gi`, cluster-default StorageClass.
- Cloud overlays may patch the size (larger, or smaller when the StorageClass uses a filesystem that allocates
  inodes dynamically, such as XFS). The kind overlay keeps the base value.
- The README deployment section documents the inode requirement (about 521k, checked against 530,000) and the
  `insufficient free inodes` error.
- Everything else in [0014](0014-kubernetes-deployment-topology.md) (one replica, `Recreate`, Postgres StatefulSet
  in kind, managed Postgres in the cloud, scaling path) is unchanged.

## Rationale

| Option | Pros | Cons |
|---|---|---|
| 10Gi PVC on every platform (chosen) | Works with the default ext4 storage class everywhere; no cloud-specific setup | About 7.5 GB provisioned but unused; slightly higher storage cost |
| Keep 5Gi with an XFS StorageClass | Smaller volume; XFS allocates inodes dynamically | Needs a custom StorageClass per cloud (`csi.storage.k8s.io/fstype: xfs`); does not work with the cluster default |
| Keep 5Gi and lower the ext4 bytes-per-inode ratio | Smaller volume | Not exposed as a standard CSI StorageClass parameter on every cloud |

Portability across clusters with default storage outweighs the extra storage cost.

## Consequences

- Positive: `/fetch` succeeds on the EKS and GKE default storage classes without extra setup, with headroom
  (about 655k inodes, 10Gi of space) above the 530,000-inode and 4 GiB pre-flight checks.
- Negative: each environment provisions 10Gi for about 2.5 GB of data.
- Follow-ups: #12 sets 10Gi in the base PVC and documents the inode note.
