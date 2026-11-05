<p align="center">
  <a href="https://solana.com">
    <img alt="Solana" src="https://i.imgur.com/IKyzQ6T.png" width="250" />
  </a>
</p>

# Solana Geyser Plugin Interface

This crate enables a plugin to be added into the Solana Validator runtime to
take actions at the time of account updates or block and transaction processing;
for example, saving the account state to an external database. The plugin must
implement the `GeyserPlugin` trait. Please see the details of the
`geyser_plugin_interface.rs` for the interface definition.

The plugin should produce a `cdylib` dynamic library, which must expose a `C`
function `_create_plugin()` that instantiates the implementation of the
interface.

The https://github.com/solana-labs/solana-accountsdb-plugin-postgres repository
provides an example of how to create a plugin which saves the accounts data into
an external PostgreSQL database.

## Agave versions

This crate is versioned independently of Agave. An interface version may be
used by several Agave releases, or may be published before Agave adopts it.
Start with the exact Agave release you are building your plugin for.

The following selected releases were checked on November 5, 2026. The interface
links point to each Agave release's manifest, and the toolchain and lockfile
links point to the same tag.

| Agave release | Interface version | Rust toolchain | Dependencies |
| --- | --- | --- | --- |
| [v4.4.0](https://github.com/anza-xyz/agave/releases/tag/v4.4.0) | [5.0.0](https://github.com/anza-xyz/agave/blob/v4.4.0/Cargo.toml) | [1.98.1](https://github.com/anza-xyz/agave/blob/v4.4.0/rust-toolchain.toml) | [Cargo.lock](https://github.com/anza-xyz/agave/blob/v4.4.0/Cargo.lock) |
| [v4.5.0-alpha.4](https://github.com/anza-xyz/agave/releases/tag/v4.5.0-alpha.4) (prerelease) | [6.0.0](https://github.com/anza-xyz/agave/blob/v4.5.0-alpha.4/Cargo.toml) | [1.99.1](https://github.com/anza-xyz/agave/blob/v4.5.0-alpha.4/rust-toolchain.toml) | [Cargo.lock](https://github.com/anza-xyz/agave/blob/v4.5.0-alpha.4/Cargo.lock) |

Interface 7.0.0 was published ahead of its adoption in
[Agave #16158](https://github.com/anza-xyz/agave/pull/16158). As of November 5,
2026, that PR is open and Agave's `master` and `v4.5` branches still use 6.0.0.
Publishing an interface version does not mean it is used by a released Agave
validator.

### Building and distributing a plugin

The plugin boundary passes Rust types and trait objects across a shared
library. Rust has no stable ABI, so matching the interface version alone does
not establish binary compatibility. Match the validator's Rust toolchain,
target, and the versions and features of dependencies used by interface types.
Use its lockfile as a reference for those shared dependencies. For a custom
validator build, use the actual build configuration. The crate's `rust-version`
field specifies its minimum supported Rust version, not the validator's build
toolchain.

Record the interface version used to build the plugin binary. Plugin release
notes should list the Agave releases the binary was tested with and identify
its build configuration. Verify compatibility with each target validator
build, including patch releases. The table above records Agave's dependencies
and does not certify a particular plugin binary.

### Updating the mapping

When Agave adopts a new interface version or publishes a new release, update
this table from that Agave tag's `Cargo.toml`, `Cargo.lock`, and
`rust-toolchain.toml`. Keep prereleases labeled and update the adoption status
when the corresponding Agave change lands.

More information about Solana is available in the [Solana documentation](https://solana.com/docs).

Still have questions?  Ask us on [Stack Exchange](https://sola.na/sse)
