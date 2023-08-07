# Releasing Restate

Restate consists of the [runtime](https://github.com/restatedev/restate), the [Typescript SDK](https://github.com/restatedev/sdk-typescript), the [documentation](https://github.com/restatedev/documentation), the [Node template generator](https://github.com/restatedev/node-template-generator) and several example repositories ([tour of Restate](https://github.com/restatedev/tour-of-restate-typescript), [ticket reservation example](https://github.com/restatedev/example-ticket-reservation-system), [food ordering example](https://github.com/restatedev/example-food-ordering), [shopping cart example](https://github.com/restatedev/example-shopping-cart-typescript) and the [Lambda greeter example](https://github.com/restatedev/example-lambda-ts-greeter)).
In order to create a full Restate release you need to:

1. [Release the runtime](#releasing-the-restate-runtime)
2. [Release the Typescript SDK](https://github.com/restatedev/sdk-typescript#releasing-the-package)
3. [Update and release the documentation](https://github.com/restatedev/documentation#releasing-the-documentation) with the newly released runtime and SDK
4. [Update and release the Node template generator](https://github.com/restatedev/node-template-generator#releasing)
5. [Update and release the tour of Restate](https://github.com/restatedev/tour-of-restate-typescript#releasing)
6. Update the examples
   * [Ticket reservation example](https://github.com/restatedev/example-ticket-reservation-system#releasing)
   * [Food ordering example](https://github.com/restatedev/example-food-ordering#releasing)
   * [Shopping cart example](https://github.com/restatedev/example-shopping-cart-typescript#releasing)
   * [Lambda greeter example](https://github.com/restatedev/example-lambda-ts-greeter#upgrading-the-sdk)

## Releasing the Restate runtime

In order to release the Restate runtime, you have to create a tag of the form `vX.Y.Z` and push it to the repository.
The tag will trigger the [release.yml](/.github/workflows/release.yml) workflow which does the following:

* Running the local tests
* Creating and pushing the Docker image with the runtime
* Creating a draft release

In order to finish the release, you have to publish it [here](https://github.com/restatedev/restate/releases).

After having created a new runtime release, you need to:

1. [Update and release the documentation](https://github.com/restatedev/documentation#upgrading-restate-runtime-version)
2. [Update the Node template generator](https://github.com/restatedev/node-template-generator#upgrading-restate-runtime)
3. Update the examples
   * [Ticket reservation example](https://github.com/restatedev/example-ticket-reservation-system#upgrading-restate-runtime)
   * [Food ordering example](https://github.com/restatedev/example-food-ordering#upgrading-the-restate-runtime)
   * [Shopping cart example](https://github.com/restatedev/example-shopping-cart-typescript#upgrading-restate-runtime)
