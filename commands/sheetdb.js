const { SlashCommandBuilder } = require('discord.js');
const axios = require('axios');

module.exports = {
    data: new SlashCommandBuilder()
        .setName('sheetdb')
        .setDescription('Send your info to Google Sheets')
        .addStringOption(option =>
            option
                .setName('name')
                .setDescription('Optional custom name to send')
                .setRequired(false)
        ),

    async execute(interaction) {
        const customName = interaction.options.getString('name');
        const nameToSend = customName || interaction.user.username;

        await interaction.reply({
            content: `✅ Sending "${nameToSend}" to Google Sheets...`,
            ephemeral: true
        });

        const sheetData = {
            data: [
                {
                    Name: nameToSend,           // Matches "Name" column in your Google Sheet
                    Number: interaction.user.id // Matches "Number" column in your Google Sheet
                }
            ]
        };

        try {
            await axios.post('https://sheetdb.io/api/v1/fejiusto9yof7', sheetData);

            await interaction.followUp({
                content: `📊 "${nameToSend}" was successfully sent to Google Sheets!`,
                ephemeral: true
            });
        } catch (error) {
            console.error('SheetDB error:', error);

            await interaction.followUp({
                content: '❌ Failed to send data to Google Sheets.',
                ephemeral: true
            });
        }
    }
};
