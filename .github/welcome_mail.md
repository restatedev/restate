Welcome to Restate. You should have received a couple of repository invites which give you access to Restate.

The main repositories you need in the beginning are:

* [restate-dist](https://github.com/restatedev/restate-dist): your starting point to set up registry access (docker, npm)
* [node-template-generator](https://github.com/restatedev/node-template-generator): your "hello world" and "getting started" for Restate
* [sdk-typescript](https://github.com/restatedev/sdk-typescript): main SDK

If you want, [join Discord](https://discord.gg/skW3AZ6uGd) to connect with other users.

Here is a quick set of steps to get started:

**(1) Get access (one-time Access Token setup while Restate is still closed access)**

* We share the code by **adding you as a collaborator to the Restate GitHub repositories**. You'll receive a few invite emails, for the repositories.
* You should then be able to access https://docs.restate.dev/, which is behind a GitHub auth wall.
* Core repos of interest are [restate-dist](https://github.com/restatedev/restate-dist) (runtime docker image), [sdk-typescript](https://github.com/restatedev/sdk-typescript), [node-template-generator](https://github.com/restatedev/node-template-generator). The remainder are examples that are just FYI.
* The same credentials that give you access to the repos also let you pull the **npm packages** and **docker images**.
* Easiest way to set up the command line tools (npm, docker), is via a **personal access token**. We describe that [here](https://github.com/restatedev/restate-dist#creating-a-personal-access-token)

This is a bit of a tedious setup, but it is a one-time thing, and it will go away as soon as Restate is publicly accessible.

Let us know if you know an easier way to grant someone access to private repos and registries.

**(2) Create your first project from a template**

The [README](https://github.com/restatedev/node-template-generator/blob/main/README.md) in the [node-template-generator](https://github.com/restatedev/node-template-generator) has the details, here is the tl;dr

* Run `npx -y @restatedev/create-app` to create a new project, install (`npm install`) and build (`npm run build`)
* The comments in the sample code should help you get started

**(3) Take the tour**

To explore more different features, take a look at the [Tour of Restate](https://docs.restate.dev/tour).

**(4) Be in touch and stay tuned for updates**

We are pushing updates regularly, so stay tuned. We usually post this on Discord.

Looking forward to hearing your feedback. Would be fun if we end up using each other's tech!

If you should encounter any problems then write me or contact us on [Discord](https://discord.gg/skW3AZ6uGd).

Best regards, Stephan