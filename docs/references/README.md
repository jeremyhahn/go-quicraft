# Reference standards

**Standards first** ([handbook documentation.md §9](../../../handbook/software/conventions/documentation.md#9-standards-first)):
where an industry standard such as an RFC exists, go-quicraft implements it
precisely and designs nothing in its place. The standard's text is copied into
this directory, and a draft is adopted at one pinned revision, which
[ROADMAP.md](../../ROADMAP.md) `## Dependencies` watches.

A standard is listed only when go-quicraft code chooses, configures or
implements it. One a library uses internally, without this code choosing it,
is not listed.

| File | Standard (title, revision) | Implementing package | Pinned draft revision |
|---|---|---|---|
| [raft.pdf](raft.pdf) | Ongaro and Ousterhout, *In Search of an Understandable Consensus Algorithm (Extended Version)*, USENIX ATC 2014 | [pkg/internal/raft](../../pkg/internal/raft) | — |
| [OngaroPhD.pdf](OngaroPhD.pdf) | Ongaro, *Consensus: Bridging Theory and Practice*, PhD dissertation, Stanford University, 2014 (PreVote, CheckQuorum, ReadIndex, leader transfer, membership change) | [pkg/internal/raft](../../pkg/internal/raft) | — |
| [rfc9000.txt](rfc9000.txt) | RFC 9000, *QUIC: A UDP-Based Multiplexed and Secure Transport*, May 2021 | [pkg/internal/transport](../../pkg/internal/transport) | — |
| [rfc9001.txt](rfc9001.txt) | RFC 9001, *Using TLS to Secure QUIC*, May 2021 (0-RTT and its §8 replay rules) | [pkg/internal/transport](../../pkg/internal/transport) | — |
| [rfc8446.txt](rfc8446.txt) | RFC 8446, *The Transport Layer Security (TLS) Protocol Version 1.3*, August 2018 | [pkg/internal/transport](../../pkg/internal/transport) | — |
| [rfc7301.txt](rfc7301.txt) | RFC 7301, *Transport Layer Security (TLS) Application-Layer Protocol Negotiation Extension*, July 2014 (ALPN `quicraft`) | [pkg/internal/transport](../../pkg/internal/transport) | — |
| [rfc7748.txt](rfc7748.txt) | RFC 7748, *Elliptic Curves for Security*, January 2016 (X25519, non-FIPS build) | [pkg/internal/transport](../../pkg/internal/transport) | — |
| [NIST.SP.800-186.pdf](NIST.SP.800-186.pdf) | NIST SP 800-186, *Recommendations for Discrete Logarithm-based Cryptography: Elliptic Curve Domain Parameters*, February 2023 (P-256, P-384) | [pkg/internal/transport](../../pkg/internal/transport) | — |
| [rfc5280.txt](rfc5280.txt) | RFC 5280, *Internet X.509 Public Key Infrastructure Certificate and Certificate Revocation List (CRL) Profile*, May 2008 | [pkg/internal/transport](../../pkg/internal/transport), [pkg/internal/transport/revocation](../../pkg/internal/transport/revocation) | — |
| [rfc6960.txt](rfc6960.txt) | RFC 6960, *X.509 Internet Public Key Infrastructure Online Certificate Status Protocol - OCSP*, June 2013 | [pkg/internal/transport/revocation](../../pkg/internal/transport/revocation) | — |
| [NIST.FIPS.197-upd1.pdf](NIST.FIPS.197-upd1.pdf) | NIST FIPS 197, *Advanced Encryption Standard (AES)*, updated May 2023 (AES-128/192/256; also the GF(2^8) reduction polynomial 0x11B) | [pkg/seal](../../pkg/seal), [pkg/crypto/shamir](../../pkg/crypto/shamir) | — |
| [nistspecialpublication800-38d.pdf](nistspecialpublication800-38d.pdf) | NIST SP 800-38D, *Recommendation for Block Cipher Modes of Operation: Galois/Counter Mode (GCM) and GMAC*, November 2007 (AES-GCM and its invocation limit) | [pkg/seal](../../pkg/seal) | — |
| [rfc8439.txt](rfc8439.txt) | RFC 8439, *ChaCha20 and Poly1305 for IETF Protocols*, June 2018 | [pkg/seal](../../pkg/seal) | — |
| [draft-irtf-cfrg-xchacha-03.txt](draft-irtf-cfrg-xchacha-03.txt) | draft-irtf-cfrg-xchacha, *XChaCha: eXtended-nonce ChaCha and AEAD_XChaCha20_Poly1305* (expired Internet-Draft) | [pkg/seal](../../pkg/seal) | -03 (January 2020) |
| [rfc5869.txt](rfc5869.txt) | RFC 5869, *HMAC-based Extract-and-Expand Key Derivation Function (HKDF)*, May 2010 | [pkg/seal](../../pkg/seal) | — |
| [rfc2104.txt](rfc2104.txt) | RFC 2104, *HMAC: Keyed-Hashing for Message Authentication*, February 1997 | [pkg/seal](../../pkg/seal) | — |
| [NIST.FIPS.180-4.pdf](NIST.FIPS.180-4.pdf) | NIST FIPS 180-4, *Secure Hash Standard (SHS)*, August 2015 (SHA-256 under HKDF, HMAC and PBKDF2) | [pkg/seal](../../pkg/seal) | — |
| [rfc9106.txt](rfc9106.txt) | RFC 9106, *Argon2 Memory-Hard Function for Password Hashing and Proof-of-Work Applications*, September 2021 (Argon2id, non-FIPS build) | [pkg/seal](../../pkg/seal) | — |
| [rfc8018.txt](rfc8018.txt) | RFC 8018, *PKCS #5: Password-Based Cryptography Specification Version 2.1*, January 2017 (PBKDF2) | [pkg/seal](../../pkg/seal) | — |
| [nistspecialpublication800-132.pdf](nistspecialpublication800-132.pdf) | NIST SP 800-132, *Recommendation for Password-Based Key Derivation, Part 1: Storage Applications*, December 2010 (PBKDF2 as the FIPS build's KDF) | [pkg/seal](../../pkg/seal) | — |
| [NIST.FIPS.140-3.pdf](NIST.FIPS.140-3.pdf) | NIST FIPS 140-3, *Security Requirements for Cryptographic Modules*, March 2019 (`RequireFIPS`, the `fips` build) | [pkg/seal](../../pkg/seal), [pkg/internal/transport](../../pkg/internal/transport) | — |
| not copied: the paper is published by the ACM and is not freely redistributable | Shamir, *How to Share a Secret*, Communications of the ACM 22(11):612–613, 1979, <https://doi.org/10.1145/359168.359176> | [pkg/crypto/shamir](../../pkg/crypto/shamir) | — |
| [rfc4648.txt](rfc4648.txt) | RFC 4648, *The Base16, Base32, and Base64 Data Encodings*, October 2006 (§4 base64, the share encoding) | [pkg/crypto/shamir](../../pkg/crypto/shamir) | — |
| [rfc7143.txt](rfc7143.txt) | RFC 7143, *Internet Small Computer System Interface (iSCSI) Protocol (Consolidated)*, April 2014 (§13.1: the CRC32C polynomial 0x11EDC6F41) | [pkg/logdb/waldb](../../pkg/logdb/waldb) | — |

The CRC32C polynomial is Castagnoli, Brauer and Herrmann, *Optimization of
Cyclic Redundancy-Check Codes with 24 and 32 Parity Bits*, IEEE Transactions on
Communications 41(6), 1993. That paper is not freely redistributable, so the
copy here is RFC 7143, which specifies the same polynomial and cites the paper.
