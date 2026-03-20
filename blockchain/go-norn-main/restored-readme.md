<!-- PROJECT SHIELDS -->
<!--
*** I'm using markdown "reference style" links for readability.
*** Reference links are enclosed in brackets [ ] instead of parentheses ( ).
*** See the bottom of this document for the declaration of the reference variables
*** for contributors-url, forks-url, etc. This is an optional, concise syntax you may use.
*** https://www.markdownguide.org/basic-syntax/#reference-style-links
-->
[![Contributors][contributors-shield]][contributors-url]
[![Forks][forks-shield]][forks-url]
[![Stargazers][stars-shield]][stars-url]
[![Issues][issues-shield]][issues-url]
[![MIT License][license-shield]][license-url]

<!-- PROJECT LOGO -->
<br />
<div align="center">

<h3 align="center">Go-Norn</h3>

  <p align="center">
    A PoVF-base blockchain node implemented with Go-lang
    <br />
    <br />
  </p>
</div>


<!-- ABOUT THE PROJECT -->
## About The Project

Norn is a blockchain node based on [Proof of Verifiable Function (PoVF)](https://doi.org/10.1016/j.comnet.2025.111092). Based on the unpredictability of verifiable 
functions, PoVF provides a fair and decentralized consensus mechanism.

<!-- GETTING STARTED -->
## Getting Started

### Build & Run node

1. Clone the repo
   ```sh
   git clone https://github.com/Chain-Lab/go-norn.git
   ```
2. Build node
   ```sh
   cd ./cmd/norn
   go build
   ```
3. generate config
   ```sh
   cd ./cmd/generate
   go build
   ./generate
   ```

4. Run a genesis norn node
   ```sh
   ./norn -d [data_path] -g -c [config_path]
   ```
5. Run general node
   ```sh
   ./norn -d [data_path] -c [config_path] -b [bootstrap_url]

## IPFS + On/Off-chain Storage

This repository now includes a local bridge command for `IPFS (off-chain data)` + `go-norn (on-chain CID)`.

- Start IPFS: `./scripts/ipfs-up.ps1` (Windows PowerShell)
- Start IPFS without Docker: `./scripts/ipfs-native-up.ps1`
- Build bridge CLI: `go build -o ./bin/ipfs-chain ./cmd/ipfs-chain`
- Use `put/get` to store CID on chain and fetch content back from IPFS

Chinese setup and end-to-end examples:
- `docs/IPFS_CHAIN_CN.md`

<!-- LICENSE -->
## License

Distributed under the MIT License. See `LICENSE.txt` for more information.

<p align="right">(<a href="#readme-top">back to top</a>)</p>


<!-- MARKDOWN LINKS & IMAGES -->
<!-- https://www.markdownguide.org/basic-syntax/#reference-style-links -->
[contributors-shield]: https://img.shields.io/github/contributors/Chain-Lab/go-norn.svg?style=for-the-badge
[contributors-url]: https://github.com/Chain-Lab/go-norn/graphs/contributors
[forks-shield]: https://img.shields.io/github/forks/Chain-Lab/go-norn.svg?style=for-the-badge
[forks-url]: https://github.com/Chain-Lab/go-norn/network/members
[stars-shield]: https://img.shields.io/github/stars/Chain-Lab/go-norn.svg?style=for-the-badge
[stars-url]: https://github.com/Chain-Lab/go-norn/stargazers
[issues-shield]: https://img.shields.io/github/issues/Chain-Lab/go-norn.svg?style=for-the-badge
[issues-url]: https://github.com/Chain-Lab/go-norn/issues
[license-shield]: https://img.shields.io/github/license/Chain-Lab/go-norn.svg?style=for-the-badge
[license-url]: https://github.com/Chain-Lab/go-norn/blob/master/LICENSE.txt
