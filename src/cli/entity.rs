use std::convert::Infallible;
use std::ops::Deref;
use std::str::FromStr;

macro_rules! identifier_arg {
    ($name:ident, $value_name:literal, $help:literal) => {
        #[derive(Clone, Debug)]
        pub(super) struct $name(String);

        impl $name {
            pub(super) const VALUE_NAME: &'static str = $value_name;
            pub(super) const HELP: &'static str = $help;
        }

        impl FromStr for $name {
            type Err = Infallible;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                Ok(Self(value.to_owned()))
            }
        }

        impl Deref for $name {
            type Target = str;

            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }
    };
}

#[derive(Clone, Debug)]
pub(super) struct OrganizationArg(String);

impl OrganizationArg {
    pub(super) const VALUE_NAME: &'static str = "ORGANIZATION";
    pub(super) const HELP: &'static str = "Organization ID or exact slug";

    pub(super) fn into_string(self) -> String {
        self.0
    }
}

impl FromStr for OrganizationArg {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if scherzo_cloud_support::valid_organization_ref(value) {
            Ok(Self(value.to_owned()))
        } else {
            Err("must be an organization ID or lowercase organization slug".to_owned())
        }
    }
}

impl Deref for OrganizationArg {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

identifier_arg!(PoolArg, "POOL", "Runner pool ID or exact name");

impl PoolArg {
    pub(super) fn into_string(self) -> String {
        self.0
    }

    pub(super) fn resolve_id(
        &self,
        api: &scherzo_cloud_api::RunnerApi,
        organization: &str,
    ) -> Result<String, scherzo_cloud_api::RunnerFailure> {
        api.get_pool(organization, self).map(|pool| pool.id)
    }
}

identifier_arg!(ProjectArg, "PROJECT", "Project ID");
identifier_arg!(
    InstallationArg,
    "INSTALLATION",
    "GitHub installation binding ID"
);
identifier_arg!(RepositoryArg, "REPOSITORY", "Provider repository ID");
